#!/usr/bin/env bash
# selfheal_chaos.sh — WAL replay self-heal end-to-end (docs/wal_selfheal_design.md).
#
# A 3-EN cluster (replication 3 → every extent on all 3 ENs). We write large
# values (1 MiB each → log_stream VPs), kill -9 the PS (no graceful flush → the
# log_stream tail is unflushed and replays on restart), then FLIP ONE BYTE in
# the slot[0] replica's on-disk extent .dat (a real bit-rot). On restart:
#
#   S1 (positive): open replay hits the corrupt OPEN tail on slot[0] →
#       A4 seal-and-rolls it → A3 cross-reads a clean replica → A5 isolates the
#       bad replica (avali=0, eversion bump). Asserts: PS opens, every ACKed key
#       reads back BYTE-EXACT (incl. the key whose value bytes were corrupted —
#       this catches the read-path filter, the I2 invariant), the extent is
#       SEALED, and slot[0]'s avali is cleared while the other two stay set.
#   S2 (negative): corrupt the SAME extent on the OTHER two replicas too →
#       kill+restart → no clean replica exists → open FAILS LOUD (the partition
#       does not serve; the PS log carries the WAL-FAILSTOP / not-self-healable
#       error). S2 intentionally destroys the data, so it runs last.
#
# Deterministic hit: the OPEN tail reads replica[0]-first, so we corrupt the
# node that extent-health reports as slot[0] (mapped to its data dir via the
# per-dir `node_id` sentinel).
#
# Usage (root not required; needs ~ a few GB on $AUTUMN_DATA_ROOT's disk):
#   ./scripts/selfheal_chaos.sh
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MGR="127.0.0.1:9001"
WORK="$(mktemp -d /tmp/selfheal_chaos.XXXXXX)"
DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-rs-selfheal}"
PSLOG="/tmp/autumn-rs-logs/ps.log"
NKEYS="${NKEYS:-12}"
AC=("$ROOT/target/release/autumn-client" --manager "$MGR")
AOP=("$ROOT/target/release/autumn-op" --manager "$MGR")
FAIL=0

say()  { echo "[selfheal $(date +%H:%M:%S)] $*"; }
fail() { echo "[selfheal $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }

cleanup_all() {
    for pid in $(ps -eo pid,comm | awk '$2 ~ /^autumn-|^etcd$/ {print $1}'); do
        kill -9 "$pid" 2>/dev/null
    done
    sleep 1
}

kill_ps() { pkill -9 -f "autumn-ps --psid 1" 2>/dev/null; sleep 2; }
start_ps() {
    env AUTUMN_DATA_ROOT="$DATA_ROOT" AUTUMN_TRANSPORT=tcp AUTUMN_METRICS=1 \
        AUTUMN_EXTENT_BASE_PORT=20000 \
        bash "$ROOT/cluster.sh" start-ps > "$WORK/start_ps.log" 2>&1
}

# extent-health JSON for one extent_id → "sealed_length slot0node slot0avali slot1avali slot2avali"
# Retries: the manager's extent-health report can transiently return empty
# (busy / mid-probe); a one-off must not abort the test.
extent_state() { # $1 = extent_id
    local parsed
    for _ in 1 2 3 4 5 6; do
        "${AOP[@]}" --json extent-health --all > "$WORK/eh.json" 2>/dev/null
        # heredoc supplies the CODE on stdin; JSON is read from the file (argv[1])
        # — never pipe JSON into a `python3 - <<HEREDOC` (stdin conflict).
        parsed=$(python3 - "$WORK/eh.json" "$1" 2>/dev/null <<'PY'
import sys,json
try:
    data=json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
eid=int(sys.argv[2])
for e in data:
    if e["extent_id"]==eid:
        s={x["slot"]:x for x in e["slots"]}
        print(e["sealed_length"], s[0]["node_id"],
              str(s[0]["avali"]).lower(), str(s[1]["avali"]).lower(), str(s[2]["avali"]).lower())
        break
PY
)
        [ -n "$parsed" ] && { printf '%s' "$parsed"; return 0; }
        sleep 2
    done
    return 1
}

# ── setup ────────────────────────────────────────────────────────────────────
# Safety: refuse to rm -rf anything that isn't an obviously-dedicated autumn
# test dir (guards a mis-set AUTUMN_DATA_ROOT pointing at a shared/real path).
case "$DATA_ROOT" in
    *autumn*[!/]) : ;;
    *) echo "refusing: AUTUMN_DATA_ROOT ('$DATA_ROOT') must be a dedicated dir whose name contains 'autumn'"; exit 1 ;;
esac
say "clean + start 3-EN / 1-partition TCP cluster (data root $DATA_ROOT)"
cleanup_all
rm -rf "$DATA_ROOT" /tmp/autumn-rs /tmp/autumn-rs-logs
env AUTUMN_DATA_ROOT="$DATA_ROOT" \
    AUTUMN_BOOTSTRAP_PRESPLIT="1:normal" \
    AUTUMN_TRANSPORT=tcp \
    AUTUMN_METRICS=1 \
    AUTUMN_EXTENT_BASE_PORT=20000 \
    bash "$ROOT/cluster.sh" start 3 > "$WORK/cluster.log" 2>&1
grep -q "bootstrap succeeded" "$WORK/cluster.log" \
    || { echo "cluster start failed"; tail -15 "$WORK/cluster.log"; cleanup_all; exit 1; }

# ── write large values (→ log_stream VPs in the open tail) ────────────────────
say "writing $NKEYS x 1 MiB values"
dd if=/dev/urandom of="$WORK/val1m" bs=1M count=1 status=none
want=$(md5sum "$WORK/val1m" | awk '{print $1}')
: > "$WORK/acked.txt"
for n in $(seq 1 "$NKEYS"); do
    if timeout 30 "${AC[@]}" put "sh-key-$n" "$WORK/val1m" >/dev/null 2>&1; then
        echo "sh-key-$n" >> "$WORK/acked.txt"
    fi
done
acked=$(wc -l < "$WORK/acked.txt")
[ "$acked" -ge "$NKEYS" ] || { fail "only $acked/$NKEYS writes ACKed"; cleanup_all; exit 1; }

# ── locate the log_stream open tail extent + its slot[0] node + data dir ──────
INFO=$("${AOP[@]}" --json info 2>/dev/null)
EID=$(echo "$INFO" | python3 -c '
import sys,json
d=json.load(sys.stdin)
log=d["partitions"][0]["log_stream_id"]
st=[s for s in d["streams"] if s["stream_id"]==log][0]
print(st["extent_ids"][-1])')   # tail = last extent of the log stream
[ -n "$EID" ] || { fail "could not resolve log_stream tail extent"; cleanup_all; exit 1; }
read SLEN S0NODE A0 A1 A2 <<<"$(extent_state "$EID")"
say "log_stream tail = extent $EID; slot[0] node=$S0NODE; pre-kill (sealed=$SLEN avali=$A0/$A1/$A2)"

# map slot[0] node_id → data dir via the per-dir node_id sentinel
S0DIR=""
for d in "$DATA_ROOT"/d*; do
    [ "$(cat "$d/node_id" 2>/dev/null)" = "$S0NODE" ] && S0DIR="$d" && break
done
[ -n "$S0DIR" ] || { fail "could not map slot0 node $S0NODE to a data dir"; cleanup_all; exit 1; }
DAT=$(find "$S0DIR" -name "extent-$EID.dat" | head -1)
[ -n "$DAT" ] || { fail "extent-$EID.dat not found under $S0DIR"; cleanup_all; exit 1; }
DSZ=$(stat -c%s "$DAT")
OFF=$(( DSZ / 2 ))   # mid-file → inside some 1 MiB value payload → CRC-mismatch (Corrupt)

# ── S1: kill -9 PS, flip ONE byte on slot[0], restart → expect self-heal ──────
say "S1: kill -9 PS, flip 1 byte @ $OFF in slot[0] dir $S0DIR (node $S0NODE)"
kill_ps
python3 - "$DAT" "$OFF" <<'PY'
import sys
p,off=sys.argv[1],int(sys.argv[2])
with open(p,"r+b") as f:
    f.seek(off); b=f.read(1); f.seek(off); f.write(bytes([b[0]^0xFF]))
PY
[ "$(stat -c%s "$DAT")" = "$DSZ" ] || fail "S1: .dat size changed — that's truncation, not content corruption"
start_ps

say "S1: assert PS isolated the corrupt replica via the manager"
if grep -q "isolated corrupt log_stream replica" "$PSLOG" 2>/dev/null; then
    say "S1: self-heal log present"
else
    fail "S1: no 'isolated corrupt log_stream replica' in $PSLOG"
    grep -E "WAL self-heal|WAL-FAILSTOP|recover_partition" "$PSLOG" 2>/dev/null | tail -5
fi

say "S1: assert zero data loss across all $acked ACKed keys (incl. the corrupted-value key)"
bad=0
while read -r k; do
    okk=0
    for a in 1 2 3; do
        got=$(timeout 30 "${AC[@]}" get "$k" 2>/dev/null | md5sum | awk '{print $1}')
        [ "$got" = "$want" ] && { okk=1; break; }
        sleep 1
    done
    [ "$okk" = 1 ] || { bad=$((bad+1)); echo "MISMATCH $k" >> "$WORK/verify.log"; }
done < "$WORK/acked.txt"
[ "$bad" = 0 ] && say "S1: zero loss ($acked keys byte-exact)" \
                || fail "S1: $bad keys lost/corrupt (read path served a bit-rotted isolated replica?)"

say "S1: assert extent $EID is now SEALED + slot[0] isolated (avali=0), others healthy"
read SLEN2 _ B0 B1 B2 <<<"$(extent_state "$EID")"
say "S1: post-heal extent $EID sealed=$SLEN2 avali=$B0/$B1/$B2"
[ "${SLEN2:-0}" -gt 0 ] || fail "S1: extent $EID not sealed after A4 seal-and-roll"
[ "$B0" = "false" ]     || fail "S1: slot[0] (corrupt node $S0NODE) was NOT isolated (avali=$B0)"
{ [ "$B1" = "true" ] && [ "$B2" = "true" ]; } || fail "S1: a healthy slot got wrongly isolated (avali=$B1/$B2)"

# ── S2 (negative): corrupt the OTHER two replicas → open must FAIL LOUD ────────
say "S2 (negative, destroys data): corrupt extent $EID on the OTHER replicas too → all corrupt → expect fail-loud"
kill_ps
# slot[0] ($S0DIR) is ALREADY corrupt from S1 — skip it (re-XOR would REVERT
# the byte). Corrupt the remaining replicas so NO clean copy survives.
for d in "$DATA_ROOT"/d*; do
    [ "$d" = "$S0DIR" ] && continue
    f=$(find "$d" -name "extent-$EID.dat" | head -1)
    [ -n "$f" ] && python3 - "$f" "$OFF" <<'PY'
import sys
p,off=sys.argv[1],int(sys.argv[2])
with open(p,"r+b") as f:
    f.seek(off); b=f.read(1); f.seek(off); f.write(bytes([b[0]^0xFF]))
PY
done
: > "$PSLOG" 2>/dev/null || true   # fresh log so the assert sees THIS open attempt
start_ps
sleep 6   # let the open attempt run + fail
if grep -qE "not self-healable|WAL-FAILSTOP|all replicas corrupt" "$PSLOG" 2>/dev/null; then
    say "S2: open failed loud as expected"
else
    fail "S2: expected a fail-loud open error in $PSLOG (all replicas corrupt)"
    grep -E "WAL|recover_partition|self-heal" "$PSLOG" 2>/dev/null | tail -6
fi
# and the partition must NOT serve a key now
if timeout 15 "${AC[@]}" get sh-key-1 >/dev/null 2>&1; then
    fail "S2: GET succeeded but the partition should be unavailable (all replicas corrupt)"
else
    say "S2: partition correctly unavailable"
fi

# ── done ──────────────────────────────────────────────────────────────────────
[ $FAIL -eq 0 ] && cleanup_all
if [ $FAIL -eq 0 ]; then say "PASS (work dir $WORK kept)"; else say "FAILED — logs in $WORK + $PSLOG"; fi
exit $FAIL
