#!/usr/bin/env bash
# fuse_rmw_chaos.sh — RMW-GET-SWALLOW regression guard (2026-06-23).
#
# Deterministically reproduces / guards against the autumn-fuse RMW corruption:
# a PARTIAL in-place overwrite (a write whose offset lands INSIDE an existing
# extent) must read-modify-write that extent's value
# (`crates/fuse/src/extent.rs::write_region`). If that read (`kv_get`) hard-
# errors during a brief PS unavailability and the error is SWALLOWED
# (`unwrap_or_default`), the code zero-fills the untouched prefix, TRUNCATES the
# untouched suffix, and `put`s the result — fabricating zeros / dropping bytes
# on a write that returns SUCCESS. The cp/append-only `fuse_chaos.sh` never
# exercises the RMW branch, so it can't catch this.
#
# Determinism: `get` and `put` each have their OWN 10-refresh (~13 s) retry
# budget. Kill the (single) PS so the RMW's `get` exhausts its budget while
# dead, then restart it at RESTART_DELAY s so the following `put` (fresh budget)
# lands. SINGLE PS + 1 partition on purpose: no migration → the post-restart
# verify read can't wedge on part_addr reconvergence (which makes the same
# trick fragile inside the 2-PS fuse_chaos.sh).
#
#   RESTART_DELAY (default 16) ∈ (get-exhaust ~13 s, put-exhaust ~26 s).
#
# PASS  = a partial overwrite during the window either lands correctly OR fails
#         with EIO, but the untouched bytes are NEVER zeroed (file intact).
# FAIL  = the overwrite reported SUCCESS yet the untouched prefix read back as
#         zeros (the bug).
#
# Usage: AUTUMN_DATA_ROOT=/data05/autumn-rmw ./scripts/fuse_rmw_chaos.sh
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MGR="127.0.0.1:9001"
MNT="/mnt/autumn-fuse-rmw"
WORK="$(mktemp -d /tmp/fuse_rmw_chaos.XXXXXX)"
RESTART_DELAY="${RESTART_DELAY:-16}"
FAIL=0

say()  { echo "[rmw $(date +%H:%M:%S)] $*"; }
fail() { echo "[rmw $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }

unmount_all() {
    local i
    for i in 1 2 3 4 5 6; do
        grep -q " $MNT " /proc/mounts || return 0
        umount -l "$MNT" 2>/dev/null
        sleep 0.2
    done
    grep -q " $MNT " /proc/mounts && { fail "could not unmount $MNT"; return 1; }
    return 0
}

export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-rmw}"
say "cleaning + starting single-PS cluster (work=$WORK)"
unmount_all
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
sleep 2
for i in $(seq 1 35); do
    busy=$(ss -tan 2>/dev/null | grep -cE ':(9001|9301|2000[0-9]) ') || true
    [ "${busy:-0}" = "0" ] && break
    sleep 2
done
rm -rf "$AUTUMN_DATA_ROOT" /tmp/autumn-rs
env AUTUMN_EXTENT_BASE_PORT=20000 AUTUMN_TRANSPORT=tcp \
    bash "$ROOT/cluster.sh" start 3 > "$WORK/cluster.log" 2>&1
grep -q "bootstrap succeeded" "$WORK/cluster.log" || { echo "cluster start failed"; tail -20 "$WORK/cluster.log"; exit 1; }
sleep 3

mount_fuse() {
    mkdir -p "$MNT"
    setsid nohup "$ROOT/target/release/autumn-fuse" \
        --manager "$MGR" --mountpoint "$MNT" --transport tcp \
        > "$WORK/fuse_$1.log" 2>&1 < /dev/null &
    for i in $(seq 1 30); do
        mountpoint -q "$MNT" && return 0
        sleep 1
    done
    fail "fuse mount did not come up ($1)"
    return 1
}
say "mounting autumn-fuse at $MNT"
mount_fuse boot || exit 1

# ── seed file F: 1 MiB extent, known pattern A ─────────────────────────────
head -c 1048576 /dev/urandom > "$WORK/A"
head -c 262144  /dev/urandom > "$WORK/B"
cp "$WORK/A" "$MNT/F.bin"
sync
cp "$WORK/A" "$WORK/EXPECT"
dd if="$WORK/B" of="$WORK/EXPECT" bs=262144 seek=1 count=1 conv=notrunc 2>/dev/null
if cmp -s "$MNT/F.bin" "$WORK/A"; then say "baseline: F.bin == A (1 MiB)"; else fail "baseline mismatch"; fi

# ── capture the live PS cmdline so we can respawn it exactly ───────────────
PSPID=$(pgrep -f "autumn-ps --psid 1" | head -1)
[ -z "$PSPID" ] && { fail "no PS found"; exit 1; }
PSCMD=$(tr '\0' ' ' < "/proc/$PSPID/cmdline")
say "PS pid=$PSPID"

# ── deterministic trigger: kill PS, partial overwrite, respawn at +DELAY ───
( sleep "$RESTART_DELAY"
  say "restarting PS (delay ${RESTART_DELAY}s)"
  setsid nohup $PSCMD > "$WORK/ps_respawn.log" 2>&1 < /dev/null &
) &
say "kill -9 PS pid=$PSPID, then PARTIAL overwrite 256K@256K"
t0=$SECONDS
kill -9 "$PSPID"
dd if="$WORK/B" of="$MNT/F.bin" bs=262144 seek=1 count=1 conv=notrunc 2>"$WORK/dd.err"
ddrc=$?
say "dd partial-overwrite rc=$ddrc after $((SECONDS-t0))s ($(tr '\n' ' ' < "$WORK/dd.err"))"

for i in $(seq 1 30); do pgrep -f "autumn-ps --psid 1" >/dev/null && break; sleep 1; done
sleep 5

# ── verdict: remount (drop daemon caches), read back ───────────────────────
say "remount + read back F.bin"
FPID=$(pgrep -f "autumn-fuse --manager" | head -1); [ -n "$FPID" ] && kill -9 "$FPID"
sleep 1; unmount_all; mount_fuse verify || exit 1

sz=$(timeout 30 stat -c%s "$MNT/F.bin" 2>/dev/null)
got=$(timeout 30 sha256sum "$MNT/F.bin" 2>/dev/null | cut -d' ' -f1)
exp=$(sha256sum < "$WORK/EXPECT" | cut -d' ' -f1)
a=$(sha256sum < "$WORK/A" | cut -d' ' -f1)
prefix_nz=$(timeout 30 dd if="$MNT/F.bin" bs=262144 count=1 2>/dev/null | tr -d '\0' | wc -c)
say "dd rc=$ddrc size=$sz prefix[0,256K)-nonzero-bytes=$prefix_nz"
say "  got=$got expect=$exp A=$a"

# the untouched prefix [0,256K) must NEVER be all zeros, regardless of dd rc
if [ "${prefix_nz:-0}" = "0" ]; then
    fail "CORRUPTION: prefix[0,256K) fabricated zeros (swallowed RMW get-error)"
elif [ "$ddrc" = "0" ] && [ "$got" != "$exp" ]; then
    fail "CORRUPTION: dd reported SUCCESS but F.bin != expected RMW result"
elif [ "$ddrc" != "0" ] && [ "$got" != "$a" ] && [ "$got" != "$exp" ]; then
    fail "dd EIO but file is neither original-A nor expected (corrupted)"
else
    if [ "$ddrc" = "0" ]; then
        say "RESULT: write landed correctly, content == expected — OK"
    else
        say "RESULT: dd returned EIO and file intact (== A) — SAFE failure (correct fixed behavior)"
    fi
fi

unmount_all
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
if [ $FAIL -eq 0 ]; then say "PASS (work dir $WORK kept)"; else say "FAILED — logs in $WORK"; fi
exit $FAIL
