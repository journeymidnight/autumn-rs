#!/usr/bin/env bash
# enospc_chaos.sh — ENOSPC-1: disk-full behavior under live writes.
#
# EN1's data dir sits on a small loopback ext4 (512 MB). A ballast file
# squeezes its headroom, then 1 MB puts run until EN1 hits ENOSPC:
#   E1: EN1's disk must classify FULL (autumn_en_disk_full=1) while
#       STAYING online (autumn_en_disk_online=1) — capacity ≠ media fault.
#   E2: writes keep succeeding (new extents allocate on EN2-4; EN1's
#       choose_disk refuses Full → manager per-RPC fallback walks).
#   E3: rm ballast → the 2 s sweep self-heals Full→Online without any
#       process restart (autumn_en_disk_full back to 0).
#   E4: every ACKed key reads back byte-exact (zero loss).
#
# Manager allocation floor is DISABLED (=0) so the EN-side ENOSPC path is
# the one exercised — the floor would otherwise soft-avoid EN1 before the
# disk ever filled (its own unit test covers that layer).
#
# Usage (root, needs loop mounts): ./scripts/enospc_chaos.sh
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MGR="127.0.0.1:9001"
WORK="$(mktemp -d /tmp/enospc_chaos.XXXXXX)"
IMG="$WORK/disk1.img"
DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-rs}"
MNT="$DATA_ROOT/d1"
AC=("$ROOT/target/release/autumn-client" --manager "$MGR")
FAIL=0

say()  { echo "[enospc $(date +%H:%M:%S)] $*"; }
fail() { echo "[enospc $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }

en1_metric() { # $1 = grep pattern; prints last numeric field or empty
    curl -s --max-time 3 http://127.0.0.1:9601/metrics 2>/dev/null \
        | grep "$1" | head -1 | awk '{print $NF}'
}

cleanup_all() {
    for pid in $(ps -eo pid,comm | awk '$2 ~ /^autumn-|^etcd$/ {print $1}'); do
        kill -9 "$pid" 2>/dev/null
    done
    sleep 1
    umount "$MNT" 2>/dev/null
}

# ── setup: small fs for EN1 ──────────────────────────────────────────────────
say "cleaning + mounting 512MB loopback fs at $MNT"
cleanup_all
rm -rf "$DATA_ROOT" /tmp/autumn-rs
mkdir -p "$MNT"
dd if=/dev/zero of="$IMG" bs=1M count=512 status=none || { echo "dd failed"; exit 1; }
mkfs.ext4 -q -F "$IMG" || { echo "mkfs failed"; exit 1; }
mount -o loop "$IMG" "$MNT" || { echo "mount failed (need root)"; exit 1; }

say "starting 4-EN cluster (metrics on, manager alloc floor disabled)"
env AUTUMN_DATA_ROOT="$DATA_ROOT" \
    AUTUMN_EXTENT_BASE_PORT=20000 \
    AUTUMN_BOOTSTRAP_PRESPLIT="8:hexstring" \
    AUTUMN_TRANSPORT=tcp \
    AUTUMN_METRICS=1 \
    AUTUMN_MGR_MIN_ALLOC_FREE_BYTES=0 \
    bash "$ROOT/cluster.sh" start 4 > "$WORK/cluster.log" 2>&1
grep -q "bootstrap succeeded" "$WORK/cluster.log" \
    || { echo "cluster start failed"; tail -10 "$WORK/cluster.log"; cleanup_all; exit 1; }

# ── ballast: squeeze EN1's headroom to ~40 MB ────────────────────────────────
free_kb=$(df -k --output=avail "$MNT" | tail -1 | tr -d ' ')
ballast_mb=$(( free_kb / 1024 - 40 ))
say "ballast: $ballast_mb MB (leaving ~40 MB free on EN1)"
dd if=/dev/zero of="$MNT/ballast" bs=1M count="$ballast_mb" status=none

# ── E1: write 1MB values until EN1 reports FULL ──────────────────────────────
# Keys carry a random hex prefix so they spread across all 8 presplit
# partitions — 8 log-stream tails, each on 3 of 4 ENs, so EN1 hosts at
# least one ACTIVE tail with P ≈ 1-(1/4)^8 (first cut used "ek-*" keys
# that all hashed to ONE partition whose tail excluded EN1 — 300 MB of
# puts never touched the small disk).
say "E1: writing 1MB values (hex-spread keys) until EN1 classifies FULL"
dd if=/dev/urandom of="$WORK/val1m" bs=1M count=1 status=none
: > "$WORK/acked.txt"
n=0 full_seen=0
while [ $n -lt 600 ]; do
    n=$((n+1))
    key="$(printf '%02x' $(( RANDOM % 256 )))-ek-$n"
    if timeout 20 "${AC[@]}" put "$key" "$WORK/val1m" >/dev/null 2>&1; then
        echo "$key" >> "$WORK/acked.txt"
    fi
    f=$(en1_metric "^autumn_en_disk_full")
    if [ "${f:-0}" = "1" ]; then full_seen=1; say "E1: EN1 FULL after $n puts"; break; fi
done
if [ "$full_seen" != "1" ]; then
    fail "E1: EN1 never reported disk_full=1 after $n puts (free: $(df -h --output=avail "$MNT" | tail -1))"
else
    on=$(en1_metric "^autumn_en_disk_online")
    [ "${on:-0}" = "1" ] || fail "E1: disk reported OFFLINE/faulted — ENOSPC misclassified as media fault (online=$on)"
fi

# ── E2: writes must keep succeeding via the other ENs ────────────────────────
say "E2: post-full write availability (20 keys, transient errors tolerated)"
ok2=0
for i in $(seq 1 20); do
    key="$(printf '%02x' $(( RANDOM % 256 )))-post-full-$i"
    if timeout 30 "${AC[@]}" put "$key" "$WORK/val1m" >/dev/null 2>&1; then
        echo "$key" >> "$WORK/acked.txt"
        ok2=$((ok2+1))
    fi
done
[ "$ok2" -ge 18 ] || fail "E2: only $ok2/20 writes succeeded after EN1 went full"
say "E2: $ok2/20 writes OK"

# ── E3: free space → Full must self-heal without restart ─────────────────────
say "E3: rm ballast → expect Full to clear via the 2s sweep"
rm -f "$MNT/ballast"
healed=0
for i in $(seq 1 15); do
    sleep 2
    f=$(en1_metric "^autumn_en_disk_full")
    if [ "${f:-1}" = "0" ]; then healed=1; say "E3: EN1 full cleared after ~$((i*2))s"; break; fi
done
[ "$healed" = "1" ] || fail "E3: disk_full did not clear within 30s of freeing space"

say "E3b: 10 more writes after heal"
ok3=0
for i in $(seq 1 10); do
    key="$(printf '%02x' $(( RANDOM % 256 )))-post-heal-$i"
    if timeout 30 "${AC[@]}" put "$key" "$WORK/val1m" >/dev/null 2>&1; then
        echo "$key" >> "$WORK/acked.txt"
        ok3=$((ok3+1))
    fi
done
[ "$ok3" -ge 9 ] || fail "E3b: only $ok3/10 writes succeeded after heal"

# ── E4: every ACKed key reads back byte-exact ────────────────────────────────
# Per-key retry (3 × 2 s): a transient routing/read failure must not be
# recorded as loss — only a key that stays wrong across retries counts
# (mirrors the chaos timeout-is-uncertain rule, read-side).
say "E4: verifying $(wc -l < "$WORK/acked.txt") ACKed keys"
want_md5=$(md5sum "$WORK/val1m" | awk '{print $1}')
bad=0
while read -r k; do
    okk=0
    for attempt in 1 2 3; do
        got=$(timeout 20 "${AC[@]}" get "$k" 2>/dev/null | md5sum | awk '{print $1}')
        [ "$got" = "$want_md5" ] && { okk=1; break; }
        sleep 2
    done
    [ "$okk" = "1" ] || { bad=$((bad+1)); echo "MISMATCH $k" >> "$WORK/verify.log"; }
done < "$WORK/acked.txt"
if [ "$bad" = "0" ]; then
    say "E4: zero loss"
else
    fail "E4: $bad ACKed keys lost/corrupted (see $WORK/verify.log)"
fi

# On FAILURE keep the cluster alive for postmortem (next run's setup
# does its own kill+umount sweep).
[ $FAIL -eq 0 ] && cleanup_all
total=$(wc -l < "$WORK/acked.txt")
if [ $FAIL -eq 0 ]; then say "PASS ($total ACKed keys, work dir $WORK kept)"; else say "FAILED — logs in $WORK"; fi
exit $FAIL
