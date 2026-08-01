#!/usr/bin/env bash
# fuse_chaos.sh — data-plane INTERFACE chaos: autumn-fuse under failover.
#
# Boots a 2-PS cluster.sh cluster (tcp), mounts autumn-fuse, runs a file
# workload through the MOUNT (the full fuse -> SDK -> PS -> EN path incl.
# the inode-lease subsystem), and injects faults:
#   PS-kill:   kill -9 the partition-holding PS → migration; file I/O through
#              the mount must resume; every previously-synced file stays byte-exact
#   MGR-kill:  kill -9 the manager + exact-cmdline respawn → file I/O resumes
#   FUSE-kill: kill -9 the fuse daemon itself + remount → all synced files persist
# Final: full manifest verification (sha256 of every synced file).
#
# Usage: AUTUMN_DATA_ROOT=/data05/autumn-rs ./scripts/fuse_chaos.sh
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MGR="127.0.0.1:9001"
MNT="/mnt/autumn-fuse-chaos"
WORK="$(mktemp -d /tmp/fuse_chaos.XXXXXX)"
PSBIN="$ROOT/target/release/autumn-ps"
AO="$ROOT/target/release/autumn-op"
FAIL=0

say()  { echo "[fuse $(date +%H:%M:%S)] $*"; }
fail() { echo "[fuse $(date +%H:%M:%S)] FAIL: $*"; FAIL=1; }

# fusermount3 is NOT installed on every host (its absence made all the
# earlier umount calls silent no-ops and the daemon mounted OVER broken
# layers). umount -l (lazy) as root handles disconnected fuse mounts;
# loop because layers can stack.
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

export AUTUMN_DATA_ROOT="${AUTUMN_DATA_ROOT:-/data05/autumn-rs}"
say "cleaning + starting cluster"
unmount_all
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
sleep 2
say "draining cluster ports"
for i in $(seq 1 35); do
    busy=$(ss -tan 2>/dev/null | grep -cE ':(9001|9301|9351|2000[0-9]) ') || true
    [ "${busy:-0}" = "0" ] && break
    sleep 2
done
rm -rf "$AUTUMN_DATA_ROOT" /tmp/autumn-rs
env AUTUMN_EXTENT_BASE_PORT=20000 \
    AUTUMN_BOOTSTRAP_PRESPLIT="4:hexstring" \
    AUTUMN_TRANSPORT=tcp \
    bash "$ROOT/cluster.sh" start 3 > "$WORK/cluster.log" 2>&1
grep -q "bootstrap succeeded" "$WORK/cluster.log" || { echo "cluster start failed"; tail -10 "$WORK/cluster.log"; exit 1; }
setsid nohup "$PSBIN" --psid 2 --port 9351 --manager "$MGR" \
    --listen 127.0.0.1 --advertise 127.0.0.1:9351 --transport tcp \
    > "$WORK/ps2.log" 2>&1 < /dev/null &
sleep 5

AOC=(timeout 20 "$AO" --manager "$MGR" --transport tcp)

# Count partitions hosted by the PS whose per-partition band starts at $1.
# autumn-op info overview prints "  part <id>  ps 127.0.0.1:<base+ord>" (space,
# not "ps=", since the b01dfa8 dashboard rework) — a bare grep -c "ps=..." matched
# nothing. Bucket by [base, base+50) (PS1=9301, PS2=9351 are 50 apart).
# Return 1 (no stdout) on a FAILED/empty probe so callers don't fold a transient
# info outage into "0 partitions" (which would pick the wrong PS / fake migration).
parts_on() { local out; out=$("${AOC[@]}" info 2>/dev/null); [ -n "$out" ] || return 1
    printf '%s\n' "$out" | grep -oE 'ps 127\.0\.0\.1:[0-9]+' \
        | grep -oE '[0-9]+$' | awk -v b="$1" '$1>=b && $1<b+50' | wc -l; }

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

# ── workload: numbered files with sha256 manifest ──────────────────────────
# writer: continuously writes files (4 KiB..256 KiB random), fsyncs (via
# close), records sha256 in the manifest ONLY after a successful close.
: > "$WORK/manifest.txt"
( i=0
  while [ ! -f "$WORK/stop" ]; do
      sz=$(( (RANDOM % 63 + 1) * 4096 ))
      f="$MNT/chaos-$i.bin"
      # The trailing fs-type check guards the fault windows where the
      # mount is (re)cycling: a cp racing the unmount lands on the BARE
      # mountpoint directory, "succeeds", and would be manifested as
      # durable — then read as missing through the next mount.
      if head -c "$sz" /dev/urandom > "$WORK/tmpf" 2>/dev/null \
         && cp "$WORK/tmpf" "$f" 2>/dev/null \
         && cmp -s "$WORK/tmpf" "$f" 2>/dev/null \
         && [ "$(stat -f -c %T "$f" 2>/dev/null)" = "fuseblk" -o "$(stat -f -c %T "$f" 2>/dev/null)" = "fuse" ]; then
          echo "$(sha256sum < "$WORK/tmpf" | cut -d' ' -f1) chaos-$i.bin" >> "$WORK/manifest.txt"
      fi
      i=$((i+1)); sleep 0.2
  done ) &
LOOP_PID=$!
sleep 5

verify_manifest() {
    local tag="$1" bad=0 total=0
    while read -r want name; do
        total=$((total+1))
        got=$(timeout 30 sha256sum "$MNT/$name" 2>/dev/null | cut -d' ' -f1)
        if [ "$got" != "$want" ]; then
            sleep 5
            got=$(timeout 30 sha256sum "$MNT/$name" 2>/dev/null | cut -d' ' -f1)
            if [ "$got" != "$want" ]; then
                fail "[$tag] $name sha mismatch/missing (after retry)"; bad=$((bad+1))
                [ $bad -ge 5 ] && { fail "(more suppressed)"; break; }
            fi
        fi
    done < "$WORK/manifest.txt"
    [ $bad -eq 0 ] && say "[$tag] manifest verify OK ($total files)"
}
file_liveness() {
    local tag="$1" ok=0 try
    for try in $(seq 1 45); do
        if echo "live-$tag" > "$MNT/live-$tag.txt" 2>/dev/null \
           && [ "$(cat "$MNT/live-$tag.txt" 2>/dev/null)" = "live-$tag" ]; then ok=1; break; fi
        sleep 2
    done
    [ $ok -eq 1 ] && say "[$tag] file liveness OK" || fail "[$tag] file I/O wedged"
}
verify_manifest "baseline"

# ── PS-kill: kill the partition-holding PS → migration under live file I/O ──
# Retry until BOTH bands probe successfully AND at least one holds a partition —
# never fold a transient probe failure into 0 (would kill the wrong PS).
p1=""; p2=""
for _ in $(seq 1 15); do
    a=$(parts_on 9301) && b=$(parts_on 9351) && [ $((a + b)) -gt 0 ] && { p1=$a; p2=$b; break; }
    sleep 2
done
[ -n "$p1" ] || fail "PS-kill: could not determine partition holder (probe failed)"
if [ "${p1:-0}" -ge 1 ]; then VID=1; VPORT=9301; else VID=2; VPORT=9351; fi
VPID=$(pgrep -f -- "--psid $VID .*--transport tcp" | head -1)
say "PS-kill: kill -9 holder PS$VID pid=${VPID:-?}"
[ -n "$VPID" ] && kill -9 "$VPID"
deadline=$((SECONDS + 90))
while [ $SECONDS -lt $deadline ]; do
    left=$(parts_on "$VPORT") || true
    [ "${left:-1}" = "0" ] && break
    sleep 2
done
say "PS-kill: migration converged"
file_liveness "f1"
verify_manifest "after-PS-kill"
say "PS-kill: respawn PS$VID"
setsid nohup "$PSBIN" --psid "$VID" --port "$VPORT" --manager "$MGR" \
    --listen 127.0.0.1 --advertise "127.0.0.1:$VPORT" --transport tcp \
    > "$WORK/ps${VID}_respawn.log" 2>&1 < /dev/null &
sleep 6

# NOTE — EN (data-plane) kill+restart is covered by the dedicated
# `scripts/fuse_en_restart_chaos.sh` (integrity-focused, quick respawn). It is
# kept SEPARATE because an EN kill stalls in-flight ops for ~30 s (the dead
# replica isn't fast-failed within the rpc/bridge 30 s window) — fine for a
# focused check, but it would make THIS continuous-workload harness slow/noisy.
# Integrity under EN restart is intact (verified there); the stall is an
# AVAILABILITY characteristic, not data loss. See fuse CLAUDE.md.

# ── MGR-kill: manager kill + respawn under live file I/O ───────────────────
MPID=$(pgrep -f autumn-manager-server | head -1)
MCMD=$(tr '\0' ' ' < "/proc/$MPID/cmdline")
say "MGR-kill: kill -9 manager pid=$MPID"
kill -9 "$MPID"
sleep 6
setsid nohup $MCMD > "$WORK/mgr_respawn.log" 2>&1 < /dev/null &
deadline=$((SECONDS + 150))
while [ $SECONDS -lt $deadline ]; do
    [ -n "$("${AOC[@]}" info 2>/dev/null)" ] && { say "MGR-kill: manager ready"; break; }
    sleep 2
done
file_liveness "f2"
verify_manifest "after-mgr-kill"

# ── FUSE-kill: kill the fuse daemon + remount → durability across interface ─
FPID=$(pgrep -f "autumn-fuse --manager" | head -1)
say "FUSE-kill: kill -9 fuse daemon pid=$FPID + remount"
[ -n "$FPID" ] && kill -9 "$FPID"
sleep 2
unmount_all
mount_fuse remount
file_liveness "f3"
verify_manifest "after-fuse-remount"

# ── stop the continuous workload BEFORE the T phases ───────────────────────
# T1/T2 unmount + remount: a cp racing the unmount writes to the BARE
# mountpoint directory, gets recorded in the manifest, and then reads as
# missing through the next mount — a harness artifact, not data loss.
touch "$WORK/stop"
wait "$LOOP_PID" 2>/dev/null
say "workload stopped: $(wc -l < "$WORK/manifest.txt") synced files"

# ── T1: truncate-shrink crash consistency (BUG-LEASE-8 ordering) ───────────
# Shrink is meta-first: the durable size commits BEFORE extents are
# destroyed. A crash mid-truncate may leave the OLD or the NEW size, but
# content[0..size] must ALWAYS equal the original prefix — the pre-fix
# ordering could leave size=old with the tail extents already destroyed,
# reading back zeros INSIDE the file.
say "T1: truncate-shrink burst + fuse kill + remount"
mkdir -p "$WORK/trunc_src"
NT=40
for i in $(seq 1 $NT); do
    head -c 1048576 /dev/urandom > "$WORK/trunc_src/t-$i"
    cp "$WORK/trunc_src/t-$i" "$MNT/trunc-$i.bin"
done
# timeout: a truncate blocked on the killed daemon must not hang the
# harness (the burst shell may sit in an uninterruptible FUSE wait).
( timeout 60 bash -c 'for i in $(seq 1 '"$NT"'); do truncate -s $(( (RANDOM % 800 + 8) * 1024 )) "'"$MNT"'/trunc-$i.bin" 2>/dev/null; done' ) &
TR_PID=$!
sleep 0.3   # land the kill mid-burst
FPID=$(pgrep -f "autumn-fuse --manager" | head -1)
say "T1: kill -9 fuse daemon pid=$FPID mid-truncate"
[ -n "$FPID" ] && kill -9 "$FPID"
wait "$TR_PID" 2>/dev/null
# the daemon was killed — the mountpoint is broken until unmounted.
unmount_all
mount_fuse t1-remount
t1_bad=0 t1_shrunk=0
for i in $(seq 1 $NT); do
    f="$MNT/trunc-$i.bin"
    sz=$(timeout 30 stat -c%s "$f" 2>/dev/null) || { fail "T1: trunc-$i.bin missing"; t1_bad=$((t1_bad+1)); continue; }
    [ "$sz" -lt 1048576 ] && t1_shrunk=$((t1_shrunk+1))
    if ! cmp -s -n "$sz" "$f" "$WORK/trunc_src/t-$i"; then
        fail "T1: trunc-$i.bin content[0..$sz] != original prefix (zeros inside file?)"
        t1_bad=$((t1_bad+1))
    fi
done
# window-hit evidence: some (not necessarily all) truncates must have
# landed before the kill, else the phase silently tested nothing.
[ "$t1_shrunk" -ge 1 ] || fail "T1: no file shrunk — kill landed before any truncate (window missed)"
[ $t1_bad -eq 0 ] && say "T1: $NT files prefix-exact at their durable size ($t1_shrunk shrunk pre-kill)"

# ── T2: shrink → grow must read ZEROS in the re-extended range ──────────────
# (BUG-LEASE-8 coco P1: leftover extents from the shrink window must never
# resurrect old data; clean_beyond_eof sweeps them on every grow path.)
say "T2: shrink -> grow zeros check"
t2_bad=0
for i in 1 2 3; do
    head -c 1048576 /dev/urandom > "$WORK/trunc_src/g-$i"
    cp "$WORK/trunc_src/g-$i" "$MNT/grow-$i.bin" || fail "T2: setup cp g-$i"
    truncate -s 262144 "$MNT/grow-$i.bin" || fail "T2: shrink g-$i"
    truncate -s 1048576 "$MNT/grow-$i.bin" || fail "T2: grow g-$i"
done
# Re-mount before verifying: discriminates daemon/KV state from kernel
# page-cache staleness (pages cached from the cp can outlive the
# shrink+grow on the same mount).
unmount_all
mount_fuse t2-verify
for i in 1 2 3; do
    # size must have actually re-grown — a failed grow would make the
    # zeros check below pass vacuously (empty tail).
    gsz=$(stat -c%s "$MNT/grow-$i.bin" 2>/dev/null)
    [ "$gsz" = "1048576" ] || { fail "T2: grow-$i.bin size=$gsz != 1048576 (grow did not commit)"; t2_bad=$((t2_bad+1)); continue; }
    # [0,256K) = original prefix; [256K,1M) = zeros
    if ! cmp -s -n 262144 "$MNT/grow-$i.bin" "$WORK/trunc_src/g-$i"; then
        fail "T2: grow-$i.bin prefix corrupted"; t2_bad=$((t2_bad+1))
    fi
    if tail -c +262145 "$MNT/grow-$i.bin" | tr -d '\0' | grep -q .; then
        fail "T2: grow-$i.bin re-extended range is NOT zeros (old data resurrected)"
        t2_bad=$((t2_bad+1))
    fi
done
[ $t2_bad -eq 0 ] && say "T2: all grow regions read zeros"

# ── T3: crash-mid-unlink reaping + rename-over extent reclaim ──────────────
# (UNLINK-1: tombstoned removal + mount-time sweep; rename-over used to
# leak the replaced file's extents UNCONDITIONALLY.)
say "T3: unlink burst + fuse kill + remount sweep"
for i in $(seq 1 15); do
    head -c 524288 /dev/urandom > "$MNT/rm-$i.bin"
done
( timeout 60 bash -c 'for i in $(seq 1 15); do rm -f "'"$MNT"'/rm-$i.bin" 2>/dev/null; done' ) &
RM_PID=$!
sleep 0.2
FPID=$(pgrep -f "autumn-fuse --manager" | head -1)
say "T3: kill -9 fuse daemon pid=$FPID mid-unlink"
[ -n "$FPID" ] && kill -9 "$FPID"
wait "$RM_PID" 2>/dev/null
unmount_all
mount_fuse t3-remount
sleep 2
# the sweep ran at Init; any survivor file just gets re-rm'd
t3_left=0
for i in $(seq 1 15); do
    [ -e "$MNT/rm-$i.bin" ] && { rm -f "$MNT/rm-$i.bin"; t3_left=$((t3_left+1)); }
done
say "T3: $t3_left files survived the kill (re-removed); sweep log:"
grep -h "unlink tombstone sweep" "$WORK"/fuse_t3-remount.log 2>/dev/null || say "T3: (no tombstones to reap — kill missed the window)"
# rename-over reclaim: overwrite a 1MiB file via mv, then unlink — all of
# the ino's extents must be gone (observable: recreate + read = no stale).
head -c 1048576 /dev/urandom > "$MNT/save-target.bin"
head -c 262144 /dev/urandom > "$WORK/trunc_src/save-new"
cp "$WORK/trunc_src/save-new" "$MNT/save-tmp.bin"
mv "$MNT/save-tmp.bin" "$MNT/save-target.bin"
sz=$(stat -c%s "$MNT/save-target.bin")
if [ "$sz" = "262144" ] && cmp -s "$MNT/save-target.bin" "$WORK/trunc_src/save-new"; then
    say "T3: rename-over content OK (262144B)"
else
    fail "T3: rename-over wrong content/size ($sz)"
fi
# POSIX same-file rename must be a NO-OP (coco P1: it used to destroy the file)
mv "$MNT/save-target.bin" "$MNT/save-target.bin" 2>/dev/null
if cmp -s "$MNT/save-target.bin" "$WORK/trunc_src/save-new"; then
    say "T3: same-path rename no-op OK"
else
    fail "T3: same-path rename destroyed the file"
fi

# NOTE — RMW (partial in-place overwrite) under PS failover is covered by the
# DEDICATED, deterministic `scripts/fuse_rmw_chaos.sh` (single-PS, no migration
# churn, so the verify read can't wedge on part_addr reconvergence the way a
# kill+respawn in THIS multi-PS harness would). It guards RMW-GET-SWALLOW: a
# swallowed RMW read-error zero-filling the untouched bytes of a file on a
# *successful* write. Kept separate on purpose — see fuse CLAUDE.md.

# ── final verification ──────────────────────────────────────────────────────
total=$(wc -l < "$WORK/manifest.txt")
say "final verify ($total synced files)"
verify_manifest "final"

unmount_all
for pid in $(ps -eo pid,comm | awk '$2 ~ /^(autumn-|etcd)/ {print $1}'); do kill -9 "$pid" 2>/dev/null; done
if [ $FAIL -eq 0 ]; then say "PASS ($total files, work dir $WORK kept)"; else say "FAILED — logs in $WORK"; fi
exit $FAIL
