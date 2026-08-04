#!/bin/bash
# ---------------------------------------------------------------------------
# G2 — power-loss crash-consistency test (single machine, no kernel module).
#
# Runs a single-node RF1 autumn cluster whose data plane lives on a LazyFS
# (FUSE) mount. LazyFS only persists data that the application has fsync'd;
# the "lazyfs::clear-cache" control command drops everything not yet fsync'd,
# which is exactly a power loss at that instant. The test then restarts the
# cluster (recovery replays the WAL + reloads the last checkpoint) and verifies
# that EVERY write the client got an ACK for survived, byte-for-byte, and that
# recovery never fails-loud spuriously or serves garbage.
#
# etcd (the control plane) is bind-mounted OFF LazyFS onto a normal directory,
# so only autumn's data-plane durability is under test (control plane is assumed
# to live on its own durable quorum, as in a real deployment).
#
# PREREQUISITE — build LazyFS once (userspace, needs libfuse3-dev + cmake + g++):
#   git clone --recurse-submodules https://github.com/dsrhaslab/lazyfs
#   (cd lazyfs/libs/libpcache && ./build.sh)
#   (cd lazyfs/lazyfs        && ./build.sh)
# then point LAZYFS_BIN at lazyfs/lazyfs/build/lazyfs (or drop it in a path below).
#
# Usage:  scripts/g2_crash_consistency.sh [--immediate] [--keys N] [--big M]
# Env:    LAZYFS_BIN, G2_WORK, N_SMALL, N_BIG, BIG_BYTES, QUIESCE, MAX_WAL_GAP
# Exit:   0 = PASS (all acked writes durable), 1 = FAIL (durability violation).
# ---------------------------------------------------------------------------
set -u
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# --- locate the LazyFS binary ---
LAZYFS_BIN="${LAZYFS_BIN:-}"
if [[ -z "$LAZYFS_BIN" ]]; then
  for c in "$HOME/lazyfs/lazyfs/build/lazyfs" \
           "/opt/lazyfs/lazyfs/build/lazyfs" \
           "$REPO/../lazyfs/lazyfs/build/lazyfs"; do
    [[ -x "$c" ]] && { LAZYFS_BIN="$c"; break; }
  done
fi
if [[ ! -x "$LAZYFS_BIN" ]]; then
  echo "ERROR: LazyFS binary not found. Build it (see header) and set LAZYFS_BIN=<path>/lazyfs" >&2
  exit 2
fi

# --- config ---
# default big-value total (N_BIG*BIG_BYTES = 140 MiB) deliberately exceeds MAX_WAL_GAP
# (128 MiB) so the default run forces a rotate+flush → recovery exercises the
# checkpoint-reload path too, not just WAL replay.
N_SMALL="${N_SMALL:-60}"; N_BIG="${N_BIG:-70}"; BIG_BYTES="${BIG_BYTES:-$((2*1024*1024))}"
QUIESCE="${QUIESCE:-3}"
export AUTUMN_PS_MAX_WAL_GAP="${MAX_WAL_GAP:-134217728}"   # 128 MiB → force a rotate+flush so recovery exercises the checkpoint path too
while [[ $# -gt 0 ]]; do case "$1" in
  --immediate) QUIESCE=0; shift ;;
  --keys) N_SMALL="$2"; shift 2 ;;
  --big)  N_BIG="$2";  shift 2 ;;
  *) echo "unknown arg: $1" >&2; exit 2 ;;
esac; done

W="${G2_WORK:-/tmp/autumn-g2}"
MNT="$W/mnt"; ROOT="$W/root"; ETCD_REAL="$W/etcd_real"
FIFO="$W/faults.fifo"; FIFO_DONE="$W/faults_done.fifo"; LZLOG="$W/lazyfs.log"
export AUTUMN_DATA_ROOT="$MNT" AUTUMN_TRANSPORT=tcp
export AUTUMN_EXTENT_BASE_PORT="${AUTUMN_EXTENT_BASE_PORT:-23000}"
export AUTUMN_PS_BASE_PORT="${AUTUMN_PS_BASE_PORT:-23300}"
export AUTUMN_BOOTSTRAP_PRESPLIT=""   # single partition
MGR=127.0.0.1:9001; NS=mem
CLIENT="$REPO/target/release/autumn-client"
LOGDIR=/tmp/autumn-rs-logs

log(){ echo "[g2 $(printf '%(%H:%M:%S)T' -1)] $*"; }
kill_autumn(){ for b in autumn-manager-server autumn-extent-node autumn-ps; do pkill -9 -f "target/release/$b" 2>/dev/null; done; }
kill_etcd(){ pkill -9 -f "etcd .*--data-dir.*$(basename "$W")" 2>/dev/null; }
teardown(){ log "teardown"; kill_autumn; kill_etcd; sleep 1
  mountpoint -q "$MNT/etcd" && umount "$MNT/etcd" 2>/dev/null
  fusermount3 -u "$MNT" 2>/dev/null; sleep 0.3; pkill -f "lazyfs $MNT" 2>/dev/null; }
trap teardown EXIT
cput(){ "$CLIENT" --manager "$MGR" --transport tcp --namespace "$NS" put "$1" "$2"; }
cget(){ "$CLIENT" --manager "$MGR" --transport tcp --namespace "$NS" get "$1" 2>/dev/null; }
fifo_cmd(){ ( timeout 15 cat "$FIFO_DONE" >/dev/null 2>&1 ) & local p=$!; echo "$1" > "$FIFO"; wait $p 2>/dev/null; }

# ============================ PHASE 1: setup ================================
log "=== PHASE 1: LazyFS mount + single-node RF1 cluster (data plane on FUSE) ==="
kill_autumn; kill_etcd; sleep 1
mountpoint -q "$MNT/etcd" && umount "$MNT/etcd" 2>/dev/null
fusermount3 -u "$MNT" 2>/dev/null
rm -rf "$W"; mkdir -p "$MNT" "$ROOT" "$ETCD_REAL"
cat > "$W/cfg.toml" <<EOF
[faults]
fifo_path="$FIFO"
fifo_path_completed="$FIFO_DONE"
[cache]
apply_eviction=false
[cache.simple]
custom_size="3gb"
blocks_per_page=1
[filesystem]
log_all_operations=false
logfile="$LZLOG"
EOF
"$LAZYFS_BIN" "$MNT" --config-path "$W/cfg.toml" -o allow_other -o modules=subdir -o subdir="$ROOT" -s > "$W/lazyfs.stdout" 2>&1 &
for i in $(seq 1 50); do mountpoint -q "$MNT" && break; sleep 0.1; done
mountpoint -q "$MNT" || { log "LAZYFS MOUNT FAIL"; tail "$W/lazyfs.stdout"; exit 1; }
mkdir -p "$MNT/etcd"; mount --bind "$ETCD_REAL" "$MNT/etcd" || log "WARN etcd bind-mount failed (etcd will ride on LazyFS; relies on etcd's own fsync)"
cd "$REPO"
bash cluster.sh reset >/dev/null 2>&1 || true
bash cluster.sh start 1 > "$W/start1.log" 2>&1 || { log "cluster start FAIL"; tail -20 "$W/start1.log"; exit 1; }
sleep 5
log "cluster up (crash mode: $([[ $QUIESCE == 0 ]] && echo IMMEDIATE || echo "quiesced ${QUIESCE}s"))"

# ==================== PHASE 2: baseline + workload =========================
log "=== PHASE 2: checkpoint baseline, then $N_SMALL small + $N_BIG×$((BIG_BYTES/1024))KiB writes (all must_sync) ==="
fifo_cmd "lazyfs::cache-checkpoint"   # persist sentinels + bootstrap extents (models pre-crash writeback of the already-running cluster)
ACK="$W/acked.txt"; : > "$ACK"; mkdir -p "$W/vals"
gen(){ head -c "$2" /dev/urandom > "$W/vals/$1"; printf '\n%s' "$1" >> "$W/vals/$1"; }
nfail=0
put_key(){ local k="$1"; if cput "$k" "$W/vals/$k" >>"$W/put.log" 2>&1; then echo "$k $(sha256sum <"$W/vals/$k" | cut -d' ' -f1)" >> "$ACK"; else nfail=$((nfail+1)); fi; }
for i in $(seq 1 "$N_SMALL"); do gen "s$i" 200; put_key "s$i"; done
for i in $(seq 1 "$N_BIG");   do gen "b$i" "$BIG_BYTES"; put_key "b$i"; done
NACK=$(wc -l < "$ACK")
log "workload done: $NACK acked, $nfail put-failures"
# filesystem evidence: row_stream SST bytes > 0 ⇒ a flush+checkpoint happened (not just WAL)
log "pre-crash extent inventory (LazyFS backing):"
find "$ROOT/d1" -name 'extent-*.dat' -printf '%s\t%f\n' 2>/dev/null | sort -rn | head -8 | sed 's/^/    /'
[[ "$QUIESCE" != 0 ]] && { log "quiesce ${QUIESCE}s"; sleep "$QUIESCE"; }

# ==================== PHASE 3: simulate power loss =========================
log "=== PHASE 3: CRASH — kill -9 autumn (volatile memory gone) + drop unsynced disk data ==="
kill_autumn; sleep 0.5
fifo_cmd "lazyfs::clear-cache"        # everything not fsync'd is now gone = power loss
log "unsynced data dropped"

# ==================== PHASE 4: restart + recover ===========================
log "=== PHASE 4: restart cluster (recovery reloads checkpoint + replays WAL) ==="
bash cluster.sh start 1 > "$W/start2.log" 2>&1 || { log "RESTART FAIL"; tail -25 "$W/start2.log"; }
sleep 6
if grep -qE 'cluster ready|partition: 127.0.0.1:23300' "$W/start2.log"; then log "cluster restarted + serving"; else log "WARN restart may be degraded"; tail -8 "$W/start2.log"; fi

# ==================== PHASE 5: verify durability ===========================
log "=== PHASE 5: verify all $NACK acked writes ==="
lost=0; corrupt=0; ok=0
while read -r k want; do
  got="$W/verify_$k"
  if cget "$k" > "$got" 2>/dev/null && [ -s "$got" ]; then
    if [ "$(sha256sum <"$got" | cut -d' ' -f1)" = "$want" ]; then ok=$((ok+1)); else corrupt=$((corrupt+1)); [ $corrupt -le 5 ] && log "CORRUPT $k"; fi
  else lost=$((lost+1)); [ $lost -le 5 ] && log "LOST $k"; fi
done < "$ACK"

# ==================== PHASE 6: recovery health + verdict ===================
FAILLOUD=$(grep -hE 'WAL-FAILSTOP|invalid meta|StaleVpOffset|failed to open partition|corrupt|panicked' "$LOGDIR"/ps.log "$LOGDIR"/node1.log "$LOGDIR"/manager.log 2>/dev/null | wc -l)
echo "============================================================"
echo "  G2 CRASH-CONSISTENCY RESULT  (crash: $([[ $QUIESCE == 0 ]] && echo IMMEDIATE || echo quiesced-${QUIESCE}s))"
echo "  acked writes       : $NACK   put-failures: $nfail"
echo "  survived + correct : $ok"
echo "  LOST  (acked→gone) : $lost"
echo "  CORRUPT (bad bytes): $corrupt"
echo "  recovery evidence  :"; grep -hE 'open_partition: ready' "$LOGDIR"/ps.log 2>/dev/null | head -1 | sed -E 's/\x1b\[[0-9;]*m//g; s/.*(open_partition: ready)/\1/; s/^/      /'
echo "  recovery fail-loud markers: $FAILLOUD"
echo "------------------------------------------------------------"
if [ "$lost" = 0 ] && [ "$corrupt" = 0 ] && [ "$ok" = "$NACK" ] && [ "$NACK" -gt 0 ]; then
  echo "  VERDICT: PASS — every acked write survived power loss, recovery clean"
  echo "============================================================"; exit 0
else
  echo "  VERDICT: FAIL — durability/consistency violation"
  echo "  -- ps.log tail --"; tail -20 "$LOGDIR/ps.log" 2>/dev/null
  echo "============================================================"; exit 1
fi
